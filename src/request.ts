import * as rlib from "request";
import * as http from "http";

export interface RequestOpt {
    host: string;
    timeout?: number;
    maxRetryCount?: number;
    maxRetryPeriod?: number;
}

export class UmqError extends Error {
    constructor(message: string) {
        super(message);
    }
}

export class UmqAPIError extends UmqError {
    message: string;
    _code: number;

    constructor(code: number, message: string) {
        super(message);
        this._code = code;
    }

    get code() {
        return this._code;
    }
}

const BASE_RETRY_PERIOD = 200;

export class RequestWrapper {
    _r: rlib.RequestAPI<rlib.Request, rlib.CoreOptions, rlib.RequiredUriUrl>;
    _maxRetryCount: number;
    _maxRetryPeriod: number;

    constructor(opt: RequestOpt) {
        this._r = rlib.defaults({
            baseUrl: opt.host,
            timeout: opt.timeout || 60 * 1000,
        });
        // max retry count default to 6 times
        this._maxRetryCount = opt.maxRetryCount === undefined ? 6 : opt.maxRetryCount;
        // max retry period default to 15 second
        this._maxRetryPeriod = opt.maxRetryPeriod === undefined ? 15000 : opt.maxRetryPeriod;
    }

    post(p: string, opts: rlib.CoreOptions): Promise<any> {
        return this._dofunc((cb)=> this._r.post(p, opts, cb));
    }

    get(p: string, opts: rlib.CoreOptions): Promise<any> {
        return this._dofunc((cb)=> this._r.get(p, opts, cb));
    }
    
    del(p: string, opts: rlib.CoreOptions): Promise<any> {
        return this._dofunc((cb) => this._r.del(p, opts, cb));
    }

    _dofunc(f: (cb: rlib.RequestCallback) => void): Promise<any> {
        return new Promise((resolv, reject) => {
            let triedCount = -1;
            let lastError: Error = null;
            let doit: () => void = null;
            // retry period starts with 200ms
            let retryPeriod = BASE_RETRY_PERIOD;
            let cb = (error: any, response: http.IncomingMessage, body: any) => {
                if (error) {
                    lastError = new UmqError(error.message);
                    setTimeout(doit, retryPeriod);
                    return;
                }
                if (response.statusCode == 200) {
                    return resolv(body);
                } else {
                    lastError = new UmqAPIError(response.statusCode, body);
                    if (response.statusCode === 500 || response.statusCode === 502 ||
                        response.statusCode === 503 || response.statusCode === 504) {
                        // retry when server have internal error
                        setTimeout(doit, retryPeriod);
                    } else {
                        reject(lastError);
                    }
                }
            }
            doit = () => {
                if (triedCount > this._maxRetryCount) {
                    reject(lastError);
                    return;
                }
                if (triedCount > 0) {
                    console.log("retry")
                }
                triedCount++;
                retryPeriod = Math.min(this._maxRetryPeriod, Math.random() * BASE_RETRY_PERIOD * Math.pow(2, triedCount));
                f(cb);
            }
            doit();
        })
    }
}