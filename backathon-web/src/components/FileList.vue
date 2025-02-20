<template>
    <li>
        <div class="filelist-directory">
            <div class="icon">
                <MdiIcon :path="mdiMenuRight" />
            </div>
            <div class="icon">
                <MdiIcon :path="mdiCheckboxBlankOutline" />
            </div>
            <div class="icon">
                <MdiIcon :path="mdiFolderOutline" />
            </div>
            <div>Filename</div>
        </div>
        <ul class="filelist-children">
            <li>Children</li>
        </ul>
    </li>
</template>

<style scoped>
.filelist-directory {
    display: flex;
    flex-wrap: wrap;
    column-gap: 1em;
    row-gap: 0.5em;
}
.filelist-children {
    margin-left: 16px;
}
</style>

<script setup lang="ts">
import { type components } from "@/schema";
import { computed, reactive, toRefs } from "vue";
import { conditionalUseQuery, useQuery } from "@/api";

import MdiIcon from "@/utils/MdiIcon.vue";
import { mdiMenuRight, mdiCheckboxBlankOutline, mdiFolderOutline } from "@mdi/js";

const props = defineProps<{
    snapshot: components["schemas"]["Snapshot"];
}>();

const { data: rootObj } = toRefs(
    useQuery("get", "/objects/{objid}", () => ({
        params: {
            path: {
                objid: props.snapshot.root,
            },
        },
    })),
);

type ObjectWithName = components["schemas"]["Object"] & { name: string };

const path = reactive([] as ObjectWithName[]);

const currentObj = computed(() =>
    path.length > 0
        ? path[path.length - 1]
        : rootObj.value
        ? { name: props.snapshot.path, ...rootObj.value }
        : null,
);

const selectedObjects = reactive(new Set() as Set<string>);

const { data: currentDirList } = toRefs(
    conditionalUseQuery(() => {
        if (currentObj.value) {
            return useQuery("get", "/objects/{objid}/ls", {
                params: {
                    path: {
                        objid: currentObj.value.objid,
                    },
                },
            });
        }
        return undefined;
    }),
);

const files = computed(() => {
    return currentDirList.value?.map(([name, obj]) => {
        return {
            name: name,
            objid: obj.objid,
            type: obj.type,
            size: obj.file_size,
            lastModified: obj.last_modified_time
                ? new Date(obj.last_modified_time)
                : null,
        };
    });
});

// get info about this snapshot, including the root object
</script>
