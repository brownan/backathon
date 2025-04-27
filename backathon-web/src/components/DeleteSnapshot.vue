<template>
    <StyledModal title="Delete Snapshot">
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
                {{ filesize(exclusiveSize) }}
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
            >
                Confirm Delete
            </button>
        </template>
    </StyledModal>
</template>

<style scoped></style>

<script setup lang="ts">
import StyledModal from "@/components/StyledModal.vue";
import { ConditionalSpinner } from "@/utils/conditionalspinner.ts";
import { filesize } from "@/utils/formatting.ts";

import type { components } from "@/schema";
import { h } from "vue";

const props = defineProps<{
    snapshotInfo: components["schemas"]["SnapshotInfo"] | undefined;
    snapshotExtendedInfo: components["schemas"]["SnapshotExtendedInfo"] | undefined;
}>();

function CodeBlock(props, { slots }) {
    return h(
        "div",
        {
            class: "box is-family-code",
        },
        [slots.default()],
    );
}
</script>
