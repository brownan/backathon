<template>
    <StyledModal>
        {{ props.text }}
        <template v-slot:buttons>
            <button
                type="button"
                class="button is-primary"
                @click="$emit('confirm')"
            >
                Confirm
            </button>
        </template>
    </StyledModal>
</template>

<script setup lang="ts">
import StyledModal from "@/components/StyledModal.vue";

const props = defineProps<{
    text: string;
}>();

defineEmits<{
    (e: "confirm"): void;
}>();
</script>
