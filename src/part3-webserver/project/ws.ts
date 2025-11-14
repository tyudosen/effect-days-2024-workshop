import { Effect, Layer, Match, Option, pipe, Schema } from "effect";
import * as M from "./model.ts";
import * as S from "./shared.ts";

export const Live = Layer.effectDiscard(
  Effect.gen(function* () {
    const current_connections = yield* S.CurrentConnections;
    const ws_server = yield* S.WsServer;

    ws_server.on("connection", (ws) => {
      let connectionName: string;

      function broadcastMessage(message: M.ServerOutgoingMessage) {
        const messageString = JSON.stringify(message);
        current_connections.forEach((conn) => conn._rawWS.send(messageString));
      }

      ws.on("message", (data) => {
        try {
          const message = JSON.parse(data.toString());

          const parsedMessage = Schema.decodeUnknownSync(
            Schema.Union(M.ServerIncomingMessage, M.StartupMessage),
          )(message);

          switch (parsedMessage._tag) {
            case "startup": {
              const { color, name } = parsedMessage;

              if (!M.colors.includes(color) || current_connections.has(name)) {
                ws.close(); // Close the connection if the color is not available or the name is already taken

                return;
              }

              connectionName = name;

              pipe(
                Effect.log(`New connection: ${name}`),
                Effect.runSync
              )

              current_connections.set(name, {
                _rawWS: ws,
                name,
                color,
                timeConnected: Date.now(),
              });

              broadcastMessage(M.Join.make({ name, color }));

              break;
            }

            case "message": {
              if (connectionName) {
                const conn = current_connections.get(connectionName);

                if (conn) {
                  broadcastMessage(
                    M.Message.make({
                      name: conn.name,
                      color: conn.color,
                      message: parsedMessage.message,
                      timestamp: Date.now(),
                    }),
                  );
                }
              }

              break;
            }
          }
        } catch (err) {

          pipe(
            Effect.logError("Failed to process message:", err),
            Effect.runSync
          )

        }
      });

      ws.on("close", () => {
        if (connectionName) {
          const conn = current_connections.get(connectionName);

          if (conn) {
            broadcastMessage(
              M.Leave.make({ name: conn.name, color: conn.color }),
            );

            current_connections.delete(connectionName);

            console.log(`Connection closed: ${connectionName}`);
          }
        }
      });
    });

    // setInterval(
    //   () => pipe(
    //     Effect.log("Current connections:", current_connections.size),
    //     Effect.runSync
    //   ),
    //   1000,
    // );
  }),
).pipe(
  Layer.provide(S.CurrentConnections.Live),
  Layer.provide(S.WsServer.Live),
);
