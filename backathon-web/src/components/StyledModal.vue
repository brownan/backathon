<template>
    <VueFinalModal
        v-model="model"
        class="modal"
        :class="model ? 'is-active' : null"
        overlay-class="modal-background"
        content-class="modal-content"
        :focus-trap="false"
    >
        <div class="box">
            <p>Modal Content</p>
            <slot></slot>
        </div>
        <button
            class="modal-close is-large"
            aria-label="close"
        ></button>
    </VueFinalModal>
</template>

<style></style>

<script setup lang="ts">
import { VueFinalModal } from "vue-final-modal";

const model = defineModel({ type: Boolean });
</script>
