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
                @option-select="submitNewRoot($event.value)"
                :pt="{
                    listContainer: { style: 'width: 100%' },
                    pcInputText: {
                        root: {
                            'data-test': 'hello world',
                            onKeyup: withKeys(() => submitNewRoot(newRootRef), ['enter']),
                        },
                    },
                }"
                :virtual-scroller-options="{ itemSize: 38 }"
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
import { computed, ref, watch, withKeys } from "vue";
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

const newRootRef = ref<string>("");
const query = ref<string>();

const autocompleteResult = useQuery(() => {
    if (query.value) {
        return {
            method: "get",
            url: "/roots/autocomplete",
            options: {
                params: {
                    query: {
                        query: query.value,
                    },
                },
            },
        };
    }
});
const suggestions = ref<string[]>([]);
watch(
    () => autocompleteResult.data,
    () => {
        if (autocompleteResult.data) {
            suggestions.value = autocompleteResult.data;
        }
    },
);

function complete(event: AutoCompleteCompleteEvent) {
    query.value = event.query;
}

function submitNewRoot(path: string) {
    console.log("Selected", path);
}
</script>
