import type {
    ErrorResponse,
    HttpMethod,
    PathsWithMethod,
    RequiredKeysOf,
    SuccessResponse,
} from "openapi-typescript-helpers";
import {
    type BodySerializer,
    type ClientOptions,
    DefaultParamsOption,
    type HeadersOptions,
    type MaybeOptionalInit,
    type ParseAs,
    type QuerySerializer,
    type QuerySerializerOptions,
    type RequestBodyOption,
} from "openapi-fetch";
import type { paths } from "./schema.d.ts";

import { type MaybeRefOrGetter } from "vue";

type ReactiveKeyedObject<Obj> = { [K in keyof Obj]: MaybeRefOrGetter<Obj[K]> };

type ReactiveParamsOption<T> = T extends {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    parameters: any;
}
    ? RequiredKeysOf<T["parameters"]> extends never
        ? { params?: ReactiveKeyedObject<T["parameters"]> }
        : { params: ReactiveKeyedObject<T["parameters"]> }
    : DefaultParamsOption;

export type ReactiveFetchOptions<Operation> = {
    enable?: MaybeRefOrGetter<boolean>;
} & ReactiveParamsOption<Operation> &
    RequestBodyOption<Operation> & {
        baseUrl?: string;
        querySerializer?: QuerySerializer<T> | QuerySerializerOptions;
        bodySerializer?: BodySerializer<T>;
        parseAs?: ParseAs;
        fetch?: ClientOptions["fetch"];
        headers?: HeadersOptions;
    } & Omit<RequestInit, "body" | "headers">;

export interface QueryState<T, E> {
    data: T | null;
    isReady: boolean;
    isFetching: boolean;
    error: E | null;
    cancel: () => void;
}
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type GetResponses<O> = O extends { responses: Record<string | number, any> }
    ? O["responses"]
    : never;

export function useQuery<
    Media extends "application/json",
    Method extends HttpMethod,
    Path extends PathsWithMethod<paths, Method>,
    Init extends MaybeOptionalInit<paths[Path], Method>,
>(
    method: Method,
    url: Path,
    init: MaybeRefOrGetter<Init>,
): QueryState<
    SuccessResponse<GetResponses<paths[Path][Method]>, Media>,
    ErrorResponse<GetResponses<paths[Path][Method]>, Media>
>;
