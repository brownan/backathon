<template>
    <div class="panel is-primary">
        <p class="panel-heading">Backup Roots</p>
        <div class="panel-block">
            <MdiIcon
                class="mr-2"
                :path="mdiPlusCircleOutline"
            />
            <AutoComplete
                class="add-new-autocomplete"
                v-model="newRootRef"
                placeholder="Add New Root (start typing a path)"
                :suggestions="suggestions"
                @complete="complete"
                :pt="{ listContainer: { style: 'width: 100%' } }"
            />
        </div>
        <div
            v-for="root in roots"
            :key="root.id"
            class="panel-block"
        >
            <button
                type="button"
                @click="deleteRoot(root.id, root.path)"
            >
                <MdiIcon
                    class="mr-2"
                    :path="mdiDelete"
                />
            </button>
            {{ root.path }}
        </div>
    </div>
</template>

<style>
.add-new-autocomplete {
    width: 100%;
}
</style>

<script setup lang="ts">
import { useQuery } from "@/api.ts";
import MdiIcon from "@/utils/MdiIcon.vue";
import { computed, ref } from "vue";
import AutoComplete, { type AutoCompleteCompleteEvent } from "primevue/autocomplete";

import { mdiDelete, mdiPlusCircleOutline } from "@mdi/js";
import { confirm } from "@/utils/confirm.ts";

const rootQuery = useQuery({
    method: "get",
    url: "/roots/",
    options: {},
});

const roots = computed(() => rootQuery.data || []);

function deleteRoot(id: number, path: string) {
    confirm(`Really delete root ${path}?`, () => {
        console.log("Deleted ", id, path);
    });
}

const newRootRef = ref<string>();

const suggestions = ref<string[]>([]);
function complete(event: AutoCompleteCompleteEvent) {
    const query: string = event.query;
    if (query.length === 0) {
        suggestions.value = [];
    } else {
        suggestions.value = [`"${query}"-1`, `"${query}"-2`, `"${query}"-3`];
    }
}
</script>
