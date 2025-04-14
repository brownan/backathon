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
                ref="autocomplete"
                v-model="newRootRef"
                placeholder="Add New Root (start typing a path)"
                :suggestions="suggestions"
                :select-on-focus="false"
                :complete-on-focus="true"
                @complete="complete"
                @option-select="onOptionSelect"
                :pt="{
                    listContainer: { style: 'width: 100%' },
                    pcInputText: {
                        root: {
                            onKeyup: withKeys(
                                (e: KeyboardEvent) => onEnter(e),
                                ['enter'],
                            ),
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
import { client } from "@/api.ts";

const autocomplete = ref<typeof AutoComplete>();

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
            if (autocomplete.value) {
                autocomplete.value.show();
            }
        } else {
            suggestions.value = [];
        }
    },
);

function complete(event: AutoCompleteCompleteEvent) {
    query.value = event.query;
}

function onOptionSelect() {
    console.log(`Option selected`, newRootRef.value);
    query.value = newRootRef.value;
}

function onEnter() {
    if (!autocomplete.value) {
        return;
    }
    if (!autocomplete.value.overlayVisible) {
        console.log("Enter pressed, creating root");
        client.POST("/roots/", {
            body: {
                path: newRootRef.value,
            },
        });
    }
}
</script>
