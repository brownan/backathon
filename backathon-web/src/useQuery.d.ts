import type {
    HttpMethod,
    MediaType,
    PathsWithMethod,
    ResponseObjectMap,
    SuccessResponse,
} from "openapi-typescript-helpers";
import type { MaybeOptionalInit } from "openapi-fetch";

import type { QueryState } from "./api.ts";
import type { MaybeRefOrGetter } from "vue";

export type useQueryType<
    Paths extends Record<string, Record<HttpMethod, object>>,
    Media extends MediaType,
> = <
    Method extends HttpMethod,
    Path extends PathsWithMethod<Paths, Method>,
    Init extends MaybeOptionalInit<Paths[Path], Method>,
>(
    method: Method,
    url: Path,
    ...init: RequiredKeysOf<Init> extends never
        ? [MaybeRefOrGetter<Init, unknown>?]
        : [MaybeRefOrGetter<Init, unknown>]
) => QueryState<SuccessResponse<ResponseObjectMap<Paths[Path][Method]>, Media>>;

export function makeUseQuery<Paths, Media>(
    client: Client<Paths, Media>,
): useQueryType<Paths, Media>;
