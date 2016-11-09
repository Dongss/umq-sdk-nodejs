
import {UmqClient, UmqClientConfig} from './UmqClient';
import * as L from './logger';

export type Config = UmqClientConfig; 

export function newUmqClient(config: UmqClientConfig): UmqClient {
  return new UmqClient({
    projectId: config.projectId,
    host: config.host,
    timeout: config.timeout,
  });
}


export function setLogger(logger: L.Logger) {
  L.setLogger(logger);
}

