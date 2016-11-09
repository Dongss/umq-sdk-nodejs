import * as request from "request";
import * as http from 'http';
import * as WebSocket from 'ws';
import * as event from 'events';
import * as urlparse from 'url';
import {logger} from './logger';

export type MessageId = string;

export type UmqClientConfig = {
  timeout: number,
  host: string,
  projectId: string,
}

enum UmqRequestErrorCode {
  HttpError = 0,
}

export class UmqRequestError extends Error {
  message: string;
  _code: number;

  constructor(code: number, message: string) {
    super()
    this._code = code;
    this.message = message
  }

  get code() {
    return this._code;
  }
}

export class UmqClient {
  _projectId: string;
  _host: string;
  _r : request.RequestAPI<request.Request, request.CoreOptions, request.RequiredUriUrl>;

  constructor(config: UmqClientConfig) {
    this._projectId = config.projectId;
    this._host = config.host;
    this._r = request.defaults({
      baseUrl: config.host,
      timeout: config.timeout || 10*1000,
    })
  }

  createProducer(producerId: string, producerToken: string): Producer {
    return new Producer(this._r, this._projectId, producerId, producerToken);
  }

  createConsumer(consumerId: string, consumerToken: string): Consumer {
    return new Consumer(this._r, this._host, this._projectId, consumerId, consumerToken);
  }

  createSubscription(consumerId: string, consumerToken: string, topic: string, prefetchCount?: number) {
    if (!prefetchCount) {
      prefetchCount = 1;
    }
    let u = urlparse.parse(this._host);
    u.protocol = "ws:"
    u.query = {permits: prefetchCount};
    u.pathname = u.path + this._projectId + '/' + topic + '/message/subscription';
    let url = urlparse.format(u)
    let authToken = consumerId + ':' + consumerToken;
    return new Subscription(url, this._r, this._projectId, topic,  authToken, prefetchCount);
  }
}

class Producer {
  _authToken: string;
  _projectId: string;
  _r : request.RequestAPI<request.Request, request.CoreOptions, request.RequiredUriUrl>;

  constructor(r: request.RequestAPI<request.Request, request.CoreOptions, request.RequiredUriUrl>, 
    projectId: string, producerId: string, producerToken: string) {
    this._authToken = producerId+":"+producerToken;
    this._r = r;
    this._projectId = projectId;
  }

  publishMessage(topic: string, message: Buffer|string): Promise<MessageId> {
    return new Promise<MessageId>((resolv, reject) => {
      this._r.post(this._projectId+"/"+topic+"/message", {
        headers: {Authorization: this._authToken},
        body: message,
      }, (error: any, response: http.IncomingMessage, body: any) => {
        if (error) {
          return reject(new UmqRequestError(UmqRequestErrorCode.HttpError, error.message))
        }
        if (response.statusCode == 200) {
          return resolv(body);
        } else {
          return reject(new UmqRequestError(response.statusCode, body))
        }
      })
    })
  }
}

interface Message {
  messageID: MessageId,
  content: string,
}

function ackMessage(r: request.RequestAPI<request.Request, request.CoreOptions, request.RequiredUriUrl>, 
  projectId: string, topic: string, messageIds: string[], authToken: string): Promise<void> {
  return new Promise<void>((resolv, reject) => {
    let uri = projectId+"/"+topic+"/message";
    r.del(uri, {
      headers: {Authorization: authToken},
      body: JSON.stringify({MessageID: messageIds}), 
    }, (error: any, response: http.IncomingMessage, body: any) => {
        if (error) {
          return reject(new UmqRequestError(UmqRequestErrorCode.HttpError, error.message))
        }
        if (response.statusCode == 200) {
          return resolv();
        } else {
          return reject(new UmqRequestError(response.statusCode, body))
        }
    })
  })

}

class Consumer {
  _r: request.RequestAPI<request.Request, request.CoreOptions, request.RequiredUriUrl>;
  _projectId: string;
  _authToken: string;

  constructor(r : request.RequestAPI<request.Request, request.CoreOptions, request.RequiredUriUrl>, 
    host: string, projectId: string, consumerId: string, consumerToken: string) {
    this._r = r;
    this._projectId = projectId;
    this._authToken = consumerId+":"+consumerToken;
  }

  getMessage(topic: string, count ?: string): Promise<Message[]> {
    return new Promise<Message[]>((resolv, reject) => {
      this._r.get(this._projectId+"/"+topic+"/message", {
        headers: {Authorization: this._authToken}
      }, (error: any, response: http.IncomingMessage, body: any) => {
        if (error) {
          return reject(new UmqRequestError(UmqRequestErrorCode.HttpError, error.message))
        }
        if (response.statusCode == 200) {
          return resolv(body);
        } else {
          return reject(new UmqRequestError(response.statusCode, body));
        }
      })
    })
  }

  ackMessage(topic: string, messageIds: string[]): Promise<void> {
    return ackMessage(this._r, this._projectId,topic, messageIds, this._authToken);
  }
}

type SubscriptionState = "closed" | "connecting" | "connected";

class Subscription extends event.EventEmitter {
  _r: request.RequestAPI<request.Request, request.CoreOptions, request.RequiredUriUrl>;
  _url: string;
  _topic: string;
  _projectId: string;
  _authToken: string;
  _ws : WebSocket;
  _prefetchCount: number;
  _state : SubscriptionState;
  _permits: number;
  _retryTimes: number;
  _retryDuration: number;


  constructor(url: string, r : request.RequestAPI<request.Request, request.CoreOptions, request.RequiredUriUrl>,
    projectId: string, topic: string, authToken: string, prefetchCount: number) {
    super();
    this._r = r;
    this._projectId = projectId;
    this._url = url;
    this._authToken = authToken;
    this._prefetchCount = prefetchCount;
    this._retryTimes = 0;
    this._retryDuration = 1;
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

  _connect() {
    this._state = "connecting";
    logger.debug("connecting to %s", this._url);
    this._ws = new WebSocket(this._url, {
      headers: {Authorization: this._authToken}
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

  _onWsError(err: Error) {
    logger.error("umq on websocket error %s", err.message);
    this.emit("error", err);
  }

  _onData(data: Buffer) {
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

  _onClosed(code: number) {
    if (this._state === "closed") {
      return;
    }
    if (code == 404) {
      this.emit("error", new Error("Invalidate resource url"));
    }
    this._reconnect();
  }

  _onWsConnected() {
    logger.info("umq client connected");
    this._state = "connected";
    this._retryTimes = 0;
    this._retryDuration = 1;
  }

  _onPong(data: Buffer) {

  }

  _reconnect() {
    if (this._state == "closed") {
      return;
    }
    if (this._retryTimes >= 5) {
      this.emit("error", new Error("Fail to connect to server"));
      this._state = "closed";
      return;
    }
    setTimeout(() => {
      logger.info("umq client start reconnecting");
      this._ws = null;
      this._connect();
      this._retryDuration = this._retryDuration * 2;
      this._retryTimes++;
    }, this._retryDuration*1000 + Math.random()*2000)
  }
}
