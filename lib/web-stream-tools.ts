/* eslint-disable @typescript-eslint/no-unused-vars */
type Data = Uint8Array | string;
type MaybeStream<T> = T;
export function readToEnd<T extends Data, R extends any = T>(
    input: MaybeStream<T>, join?: (chunks: T[]) => R
    // @ts-ignore
): Promise<R> { return input; }

export function isStream(input: any) { return false; }

export function passiveClone(input) { return input; }
export function clone(input) { return input; }
