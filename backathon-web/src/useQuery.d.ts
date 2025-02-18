import type {
    ErrorResponse,
    HttpMethod,
    PathsWithMethod,
    SuccessResponse,
} from "openapi-typescript-helpers";
import { type MaybeOptionalInit } from "openapi-fetch";
import type { paths } from "./schema.d.ts";

import { type MaybeRefOrGetter } from "vue";

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
