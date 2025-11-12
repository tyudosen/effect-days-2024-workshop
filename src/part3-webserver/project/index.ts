import { createServer } from "node:http";
import { Effect, Layer, Match, Option, pipe, Schema } from "effect";
import { type WebSocket, WebSocketServer } from "ws";

import * as M from "./model.ts";
import * as S from "./shared.ts";
import { BunRuntime } from "@effect/platform-bun";

const currentConnections: Map<string, M.WebSocketConnection> = new Map();

const Services = Layer.mergeAll(
  S.HttpServer.Live,
  S.WsServer.Live
)

const MainLayer = Layer.effectDiscard(Effect.gen(function* () {
  function broadcastMessage(message: M.ServerOutgoingMessage) {
    const messageString = JSON.stringify(message);
    currentConnections.forEach((conn) => conn._rawWS.send(messageString));
  }

  const http_server = yield* S.HttpServer;
  const ws_server = yield* S.WsServer;

  http_server.on('request', (req, res) => {
    const url = Option.fromNullable(req.url);

    Option.match(url, {
      onNone: () => '',
      onSome: (url) => Match.value(url).pipe(
        Match.when('/colors', () => {
          const currentColors = Array.from(currentConnections.values()).map(
            (conn) => conn.color
          );

          const availableColors = M.colors.filter(
            (color) => !currentColors.includes(color)
          );

          const message = M.AvailableColorsResponse.make({
            colors: availableColors,
          });

          res.writeHead(200, { "Content-Type": "application/json" });

          res.end(JSON.stringify(message));

        }),
        Match.orElse((url) => {
          console.log('url -->', url)
          res.writeHead(404);
          res.end("Not Found Again Option");
          return;
        })
      )
    })

  })

  // end http_server

  ws_server.on('connection', (ws) => {
    let connectionName: string;

    ws.on("message", (data) => {
      try {
        const message = JSON.parse(data.toString());

        const parsedMessage = Schema.decodeUnknownSync(
          Schema.Union(M.ServerIncomingMessage, M.StartupMessage)
        )(message);

        switch (parsedMessage._tag) {
          case "startup": {
            const { color, name } = parsedMessage;

            if (!M.colors.includes(color) || currentConnections.has(name)) {
              ws.close(); // Close the connection if the color is not available or the name is already taken

              return;
            }

            connectionName = name;

            console.log(`New connection: ${name}`);

            currentConnections.set(name, {
              _rawWS: ws,
              name,
              color,
              timeConnected: Date.now(),
            });

            broadcastMessage({ _tag: "join", name, color });

            break;
          }

          case "message": {
            if (connectionName) {
              const conn = currentConnections.get(connectionName);

              if (conn) {
                broadcastMessage({
                  _tag: "message",
                  name: conn.name,
                  color: conn.color,
                  message: parsedMessage.message,
                  timestamp: Date.now(),
                });
              }
            }

            break;
          }
        }
      } catch (err) {
        console.error("Failed to process message:", err);
      }
    });

    ws.on("close", () => {
      if (connectionName) {
        const conn = currentConnections.get(connectionName);

        if (conn) {
          broadcastMessage({ _tag: "leave", name: conn.name, color: conn.color });

          currentConnections.delete(connectionName);

          console.log(`Connection closed: ${connectionName}`);
        }
      }
    });
  })

  // end ws_server

  setInterval(
    () => console.log("Current connections:", currentConnections.size),
    1000
  );

}))
  .pipe(
    Layer.merge(S.Listen),
    Layer.provide(Services)
  )


pipe(
  MainLayer,
  Layer.launch,
  BunRuntime.runMain
)
