import { ReadableStream } from 'web-streams-polyfill';
import type { WebStream, Data } from '../api.models';

type ChunkWithData = { done: boolean, value: Data };
enum STREAM_CONTROL_TYPE {
    'READ', 'CANCEL'
}
// Transfer a readable stream chunk by chunk using message channels
export const ReadableStreamSerializer = {
    canHandle: (obj: any): obj is ReadableStream => typeof obj === 'object' && obj.getReader,
    serialize: (readableStream: WebStream<Data>): MessagePort => {
        const { port1, port2 } = new MessageChannel();

        // wait to get the reader until the first chunk is requested
        // in case the user wants to cancel the stream before starting reading it
        let reader: ReturnType<typeof readableStream.getReader> | null = null;

        port1.onmessage = async ({ data: { type } }) => {
            let dataChunk;
            switch (type) {
                case STREAM_CONTROL_TYPE.READ:
                    if (reader === null) {
                        reader = readableStream.getReader();
                    }
                    dataChunk = await reader.read();
                    port1.postMessage(
                        dataChunk,
                        dataChunk instanceof Uint8Array ? [dataChunk] : [] // transferables
                    );
                    break;
                case STREAM_CONTROL_TYPE.CANCEL:
                    if (reader) {
                        reader.cancel();
                    } else {
                        readableStream.cancel();
                    }
                    break;
                default:
                    throw new Error('Unknown stream transfer control type');
            }
        }

        // Transfer the message channel to the caller's execution context
        return port2; // NB: the port is transferable and must be transferred
    },
    deserialize: (port: MessagePort): ReadableStream => {
        // Convenience function to allow us to use async/await for messages coming down the port
        const nextPortMessage = () => new Promise<ChunkWithData>((resolve) => {
            port.onmessage = ({ data: chunk }: { data: ChunkWithData }) => {
                resolve(chunk);
            };
        });

        // Minimal proxy reader
        const proxyReader = {
            read: () => {
              port.postMessage({ type: STREAM_CONTROL_TYPE.READ });
              // promise that will resolve with the chunk returned by the remote reader
              return nextPortMessage();
            },

            cancel: () => {
                port.postMessage({ type: STREAM_CONTROL_TYPE.CANCEL })
            }
        };

        const reconstructedStream = new ReadableStream<Data>({
            async start(controller) {
                // eslint-disable-next-line no-constant-condition
                while(true) {
                    const { done, value } = await proxyReader.read();
                    // When no more data needs to be consumed, close the stream
                    if (done) {
                        controller.close();
                        return;
                    }
                    // Enqueue the next data chunk into our target stream
                    controller.enqueue(value);
                }
            },
            cancel() {
                proxyReader.cancel()
            }
        });

        return reconstructedStream;
    }
};

export type SerializeWebStreamTypes<T> = {
    [I in keyof T]: T[I] extends WebStream<Data> | undefined ? MessagePort : T[I]
};
