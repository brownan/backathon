<template>
    <StyledModal
        v-model="model"
        title="Delete Snapshot"
    >
        <div class="block">
            Really delete snapshot of
            <CodeBlock>
                <ConditionalSpinner
                    :value="props.snapshotInfo"
                    v-slot="{ value: { path } }"
                >
                    {{ path }}
                </ConditionalSpinner>
            </CodeBlock>
        </div>

        <div class="block">
            taken
            <ConditionalSpinner
                :value="props.snapshotInfo"
                v-slot="{ value: { timestamp } }"
            >
                {{ new Date(timestamp).toLocaleString() }} </ConditionalSpinner
            >?
        </div>

        <div class="block">
            This will free an estimated
            <ConditionalSpinner
                :value="props.snapshotExtendedInfo"
                v-slot="{ value: { exclusiveSize } }"
            >
                {{ formatFilesize(exclusiveSize) }}
            </ConditionalSpinner>
        </div>
        <div class="block">
            Note that space will not be freed immediately. Run garbage collection after
            removing snapshots to free space in the repository.
        </div>
        <template #buttons>
            <button
                type="button"
                class="button is-danger"
                :disabled="!props.snapshotInfo"
                @click="
                    props.snapshotInfo
                        ? deleteSnapshot(props.snapshotInfo.id)
                        : () => null
                "
            >
                Confirm Delete
            </button>
        </template>
    </StyledModal>
</template>

<style scoped></style>

<script setup lang="ts">
import StyledModal from "@/components/StyledModal.vue";
import ConditionalSpinner from "@/components/ConditionalSpinner.ts";
import CodeBlock from "@/components/CodeBlock.ts";
import { formatFilesize } from "@/utils/formatting.ts";
import { client } from "@/api";

import type { components } from "@/schema";

const props = defineProps<{
    snapshotInfo: components["schemas"]["SnapshotInfo"] | undefined;
    snapshotExtendedInfo: components["schemas"]["SnapshotExtendedInfo"] | undefined;
}>();

function deleteSnapshot(id: number) {
    client.DELETE("/snapshots/{id}", {
        params: {
            path: {
                id: id,
            },
        },
    });
    model.value = false;
}

const model = defineModel({ type: Boolean });
</script>
