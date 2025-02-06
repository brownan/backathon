import createClient from "openapi-fetch";
import { type Ref } from "vue";

import type { paths } from "./schema.d.ts";
import { makeUseQuery } from "@/useQuery";

export interface AppError {
    code: number;
    message: string;
}

export interface QueryState<T> {
    state: Ref<T | undefined>;
    isReady: Ref<boolean>;
    isFetching: Ref<boolean>;
    error: Ref<AppError | undefined>;
    execute: () => null;
}

export const client = createClient<paths, "application/json">({ baseUrl: "/api" });

export const useQuery = makeUseQuery<paths, "application/json">(client);
