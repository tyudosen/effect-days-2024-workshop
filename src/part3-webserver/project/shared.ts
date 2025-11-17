import { Context, Effect, Layer } from "effect";
import { createServer } from "node:http";
import * as M from "./model.ts";

export class HttpServer extends Context.Tag("Http")<
  HttpServer,
  ReturnType<typeof createServer>
>() {
  static readonly Live = Layer.sync(HttpServer, createServer);
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
