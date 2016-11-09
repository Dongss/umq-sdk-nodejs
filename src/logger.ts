import * as util from 'util';

export interface Logger {
  debug(fmt: string, ...args: any[]): void;
  info(fmt: string, ...args: any[]): void;
  error(fmt: string, ...args: any[]): void;
}

export function setLogger(l: Logger) {
  logger = l;
}

export var logger: Logger = {
  debug: function(fmt: string, ...args: any[]) {
    console.log(util.format(fmt, args));
  },

  info: function(fmt: string, ...args: any[]) {
    console.log(util.format(fmt, args));
  },

  error(fmt: string, ...args: any[]) {
    console.error(util.format(fmt, args));
  }
} 
