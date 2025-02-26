<template>
    <VueFinalModal
        v-model="model"
        class="modal"
        :class="model ? 'is-active' : null"
        overlay-class="modal-background"
        content-class="modal-content"
        :focus-trap="false"
        overlay-transition="vfm-fade"
        content-transition="vfm-fade"
    >
        <div class="card">
            <header
                class="card-header"
                v-if="props.title"
            >
                <p class="card-header-title">
                    <span class="title is-3">{{ props.title }}</span>
                </p>
            </header>
            <div class="card-content">
                <div class="content">
                    <slot></slot>
                    <div class="field is-grouped is-grouped-right mt-3">
                        <div class="control">
                            <button
                                @click="model = false"
                                class="button is-link is-outlined"
                            >
                                Cancel
                            </button>
                        </div>
                        <div class="is-flex-grow-1"></div>
                        <slot name="buttons"></slot>
                    </div>
                </div>
            </div>
        </div>
        <button
            class="modal-close is-large"
            aria-label="close"
            @click="model = false"
        ></button>
    </VueFinalModal>
</template>

<style></style>

<script setup lang="ts">
import { VueFinalModal } from "vue-final-modal";

const model = defineModel({ type: Boolean });
const props = defineProps({
    title: {
        required: false,
        type: String,
    },
});
</script>
