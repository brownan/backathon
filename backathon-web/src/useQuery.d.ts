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
    type ParseAs,
    type QuerySerializer,
    type QuerySerializerOptions,
    type RequestBodyOption,
} from "openapi-fetch";
import type { paths } from "./schema.d.ts";

import { type MaybeRefOrGetter, type Ref } from "vue";

type ReactiveKeyedObject<Obj> = { [K in keyof Obj]: Obj[K] | MaybeRefOrGetter<Obj[K]> };

type ReactiveParamsOption<T> = T extends {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    parameters: any;
}
    ? RequiredKeysOf<T["parameters"]> extends never
        ? { params?: ReactiveKeyedObject<T["parameters"]> }
        : { params: ReactiveKeyedObject<T["parameters"]> }
    : DefaultParamsOption;

export type ReactiveFetchOptions<Operation> = {
    enable?: Ref<boolean>;
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
    data: Ref<T | null>;
    isReady: Ref<boolean>;
    isFetching: Ref<boolean>;
    error: Ref<E | null>;
}
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type GetResponses<O> = O extends { responses: Record<string | number, any> }
    ? O["responses"]
    : never;

export function useQuery<
    Media extends "application/json",
    Method extends HttpMethod,
    Path extends PathsWithMethod<paths, Method>,
>(
    method: Method,
    url: Path,
    options: ReactiveFetchOptions<paths[Path][Method]>,
): QueryState<
    SuccessResponse<GetResponses<paths[Path][Method]>, Media>,
    ErrorResponse<GetResponses<paths[Path][Method]>, Media>
>;
