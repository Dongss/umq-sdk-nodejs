import * as http from 'http';
import * as WebSocket from 'ws';
import * as event from 'events';
import * as urlparse from 'url';
import * as request from "./request"
import { logger } from './logger';

export type MessageId = string;

export interface UmqClientConfig extends request.RequestOpt {
  projectId: string,
}

export interface UmqExtSettings {
  prefetchCount?: number,
  retryInterval?: number,
  retryMaxCount?: number,
  retryMaxTime?: number,
}

enum UmqRequestErrorCode {
  HttpError = 0,
}


export class UmqClient {
  private _projectId: string;
  private _host: string;
  private _r: request.RequestWrapper;

  constructor(config: UmqClientConfig) {
    this._projectId = config.projectId;
    this._host = config.host;
    this._r = new request.RequestWrapper(config);

  }

  createProducer(producerId: string, producerToken: string): Producer {
    return new Producer(this._r, this._projectId, producerId, producerToken);
  }

  createConsumer(consumerId: string, consumerToken: string): Consumer {
    return new Consumer(this._r, this._host, this._projectId, consumerId, consumerToken);
  }

  createSubscription(consumerId: string, consumerToken: string, topic: string, settings?: number | UmqExtSettings) {
    let prefetchCount: number = 1;
    let retryInterval: number = 200;
    let retryMaxCount: number = 8;
    let retryMaxTime: number = 30000;

    if (typeof settings === 'number') {
      prefetchCount = settings || prefetchCount;
    } else {
      settings = settings || {};
      prefetchCount = settings.prefetchCount || prefetchCount;
      retryInterval = settings.retryInterval || retryInterval;
      retryMaxCount = settings.retryMaxCount || retryMaxCount;
      retryMaxTime = settings.retryMaxTime || retryMaxTime;
    }

    let u = urlparse.parse(this._host);
    u.protocol = "ws:"
    u.query = { permits: prefetchCount };
    u.pathname = u.path + this._projectId + '/' + topic + '/message/subscription';
    let url = urlparse.format(u)
    let authToken = consumerId + ':' + consumerToken;
    return new Subscription(url, this._r, this._projectId, topic, authToken, prefetchCount, retryInterval, retryMaxCount, retryMaxTime);
  }
}

export class Producer {
  private _authToken: string;
  private _projectId: string;
  private _r: request.RequestWrapper;

  constructor(r: request.RequestWrapper,
    projectId: string, producerId: string, producerToken: string) {
    this._authToken = producerId + ":" + producerToken;
    this._r = r;
    this._projectId = projectId;
  }

  publishMessage(topic: string, message: Buffer | string): Promise<MessageId> {
    return this._r.post(this._projectId + "/" + topic + "/message", {
      headers: { Authorization: this._authToken },
      body: message,
    }).then((body: any) => {
      if (typeof body === 'string') {
        body = JSON.parse(body)
      }
      return body["MessageID"];
    });
  }
}

export interface Message {
  messageID: MessageId,
  content: string,
}

function ackMessage(r: request.RequestWrapper,
  projectId: string, topic: string, messageIds: string[], authToken: string): Promise<void> {
    return r.del(projectId + "/" + topic + "/message", {
      headers: { Authorization: authToken },
      body: JSON.stringify({ MessageID: messageIds })
    }).then((body: any) => {
      return;
    });
}

export class Consumer {
  private _r: request.RequestWrapper;
  private _projectId: string;
  private _authToken: string;

  constructor(r: request.RequestWrapper,
    host: string, projectId: string, consumerId: string, consumerToken: string) {
    this._r = r;
    this._projectId = projectId;
    this._authToken = consumerId + ":" + consumerToken;
  }

  getMessage(topic: string, count?: string, timeout?: number): Promise<Message[]> {
    let p = this._projectId + "/" + topic + "/message";
    if (count !== undefined) {
      p = p + "?count=" + count;
    } else {
      p = p + "?count=1";
    }
    if (timeout !== undefined) {
      p = p + "&timeout=" + timeout;
    }
    return this._r.get(p, {
      headers: { Authorization: this._authToken },
      
    }).then((body: any) => {
      if (typeof body === 'string') {
        body = JSON.parse(body)
      }
      console.log(body);
      return body.messages;
    });
  }

  ackMessage(topic: string, messageIds: string[]): Promise<void> {
    return ackMessage(this._r, this._projectId, topic, messageIds, this._authToken);
  }
}

type SubscriptionState = "closed" | "connecting" | "connected";

export class Subscription extends event.EventEmitter {
  private _r: request.RequestWrapper;
  private _url: string;
  private _topic: string;
  private _projectId: string;
  private _authToken: string;
  private _ws: WebSocket;
  private _prefetchCount: number;
  private _state: SubscriptionState;
  private _permits: number;
  private _retryTimes: number;
  private _retryDuration: number;
  private _retryInterval: number;
  private _retryMaxCount: number;
  private _retryMaxTime: number;


  constructor(url: string, r: request.RequestWrapper,
    projectId: string, topic: string, authToken: string, prefetchCount: number,
    retryInterval: number, retryMaxCount: number, retryMaxTime: number) {
    super();
    this._r = r;
    this._projectId = projectId;
    this._url = url;
    this._authToken = authToken;
    this._prefetchCount = prefetchCount;
    this._retryTimes = 0;
    this._retryDuration = 1;
    this._retryInterval = retryInterval;
    this._retryMaxCount = retryMaxCount;
    this._retryMaxTime = retryMaxTime;
    this._topic = topic;
    this._connect()
  }

  ackMessage(messageIds: string[]) {
    return ackMessage(this._r, this._projectId, this._topic, messageIds, this._authToken);
  }

  close() {
    if (this._ws) {
      this._state = "closed";
      this._ws.close();
    }
    this.removeAllListeners();
  }

  private _connect() {
    this._state = "connecting";
    logger.debug("connecting to %s", this._url);
    this._ws = new WebSocket(this._url, {
      headers: { Authorization: this._authToken }
    });
    this._prefetchCount = this._prefetchCount;
    this._ws.on('error', err => this._onWsError(err));
    this._ws.on("message", (data, flags) => {
      this._onData(data);
    });
    this._ws.on("close", (code, message) => {
      this._onClosed(code);
    });
    this._ws.on("open", () => this._onWsConnected());
    this._ws.on("pong", (data, flags) => this._onPong(data));
  }

  private _onWsError(err: Error) {
    logger.error("umq on websocket error %s", err.message);
    this.emit("error", err);
  }

  private _onData(data: Buffer) {
    let b = data.toString("utf8");
    logger.debug("umq on server data %s", b);
    try {
      let message = JSON.parse(b) as Message;
      this.emit("data", message);
    } catch (e) {
      this.emit("error", new Error("Invalid data received"));
      this.close();
    }
  }

  private _onClosed(code: number) {
    if (this._state === "closed") {
      return;
    }
    if (code == 404) {
      this.emit("error", new Error("Invalidate resource url"));
    }
    this._reconnect();
  }

  private _onWsConnected() {
    logger.info("umq client connected");
    this._state = "connected";
    this._retryTimes = 0;
    this._retryDuration = 0;
  }

  private _onPong(data: Buffer) {

  }

  private _reconnect() {
    if (this._state == "closed") {
      return;
    }
    if (this._retryTimes >= this._retryMaxCount) {
      this.emit("error", new Error("Fail to connect to server"));
      this._state = "closed";
      return;
    }
    let waitTime = Math.min(this._retryMaxTime, Math.random() * this._retryInterval * Math.pow(2, this._retryTimes));
    setTimeout(() => {
      logger.info("umq client start reconnecting");
      this._ws = null;
      this._connect();
      this._retryDuration += waitTime;
      this._retryTimes++;
    }, waitTime);
  }
}
