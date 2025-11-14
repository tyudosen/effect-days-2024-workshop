import { Context, Effect, Layer } from "effect";
import { createServer } from "node:http";
import * as M from "./model.ts";
import { WebSocketServer } from "ws";

export class HttpServer extends Context.Tag("Http")<
  HttpServer,
  ReturnType<typeof createServer>
>() {
  static readonly Live = Layer.sync(HttpServer, createServer);
}

export class WsServer extends Context.Tag("WsServer")<
  WsServer,
  WebSocketServer
>() {
  static readonly Live = Layer.effect(
    WsServer,
    Effect.gen(function* () {
      const wss = new WebSocketServer({ noServer: true });

      return wss;
    }),
  );
}


export class CurrentConnections extends Context.Tag("CurrentConnections")<
  CurrentConnections,
  Map<string, M.WebSocketConnection>
>() {
  static readonly Live = Layer.sync(CurrentConnections, () => new Map());
}

export const getAvailableColors = Effect.gen(function* () {
  const current_connections = yield* CurrentConnections;

  const currentColors = Array.from(current_connections.values()).map(
    (conn) => conn.color,
  );

  const availableColors = M.colors.filter(
    (color) => !currentColors.includes(color),
  );

  return availableColors;
});
