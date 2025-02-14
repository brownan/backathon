import createClient from "openapi-fetch";

import type { paths } from "./schema.d.ts";

export const client = createClient<paths, "application/json">({ baseUrl: "/api" });

export { useQuery } from "@/useQuery";
