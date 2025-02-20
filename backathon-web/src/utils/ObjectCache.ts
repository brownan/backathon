import type { components } from "@/schema";
import type { Ref } from "vue";
import { useQuery } from "@/api.ts";
import { readonly, toRef } from "vue";

export class ObjectCache {
    objMap: Map<string, Ref<components["schemas"]["Object"] | null>> = new Map();

    getObject(objid: string): Ref<components["schemas"]["Object"] | null> {
        const existing = this.objMap.get(objid);
        if (existing) {
            return existing;
        }

        const queryState = useQuery("get", "/objects/{objid}", {
            params: {
                path: {
                    objid: objid,
                },
            },
        });
        const newRef = readonly(toRef(queryState, "data"));
        this.objMap.set(objid, newRef);
        return newRef;
    }
}
