enum PROMISE_CONTROL_TYPE {
  'RESOLVE', 'REJECT'
}
export interface PromiseControlData<T = any> {
  type: PROMISE_CONTROL_TYPE,
  value: T | Error
}
/**
 * Promises are not natively transferred by comlink.
 * To tranfer them, this handler creates a message channel, than listens to requests to resolve the promise, and posts the awaited value.
 * NB: currently, this only supports promises that return types that can be transferred as-is (without requiring other custom handlers).
*/
export const PromiseSerializer = {
    canHandle: (obj: any): obj is Promise<any> => typeof obj === 'object' && obj.then,
    serialize: <T extends any>(promise: Promise<T>): MessagePort => {
        const { port1, port2 } = new MessageChannel();

        port1.onmessage = async () => {
            // promise
            //     .then((value) => port1.postMessage({ type: PROMISE_CONTROL_TYPE.RESOLVE, value }))
            //     .catch((error) => port1.postMessage({ type: PROMISE_CONTROL_TYPE.REJECT, value: error }))
            try {
                const value = await promise;
                port1.postMessage({ type: PROMISE_CONTROL_TYPE.RESOLVE, value })
            } catch(err) {
                port1.postMessage({ type: PROMISE_CONTROL_TYPE.REJECT, value: err })
            }
        }

        // Transfer the message channel to the caller's execution context
        return port2; // NB: the port is transferable and must be transferred
    },

    deserialize: <T extends any>(port: MessagePort): Promise<T> => {
        const nextPortMessage = () => new Promise<PromiseControlData<T>>((resolve) => {
            port.onmessage = ({ data }: { data: PromiseControlData<T> }) => {
                resolve(data);
            };
        });

        const proxyPromise = (async () => {
            port.postMessage({});
            const { type, value } = await nextPortMessage();
            switch(type) {
                case PROMISE_CONTROL_TYPE.RESOLVE:
                    return value as T;
                case PROMISE_CONTROL_TYPE.REJECT:
                    throw value as Error;
                default:
                    throw new Error('Unexpected promise control message type');
            }
        })();

        return proxyPromise;
    }
};
