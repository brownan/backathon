<!--
This component is adapted from the @jamescoyle/vue-icon package

MIT License

Copyright (c) 2020 James Coyle

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
-->
<template>
    <svg
        :width="sizeValue"
        :height="sizeValue"
        :viewBox="viewboxValue"
        :style="styles"
    >
        <path :d="path" />
    </svg>
</template>

<style scoped>
svg {
    /*noinspection CssUnresolvedCustomProperty*/
    transform: rotate(var(--r, 0deg)) scale(var(--sx, 1), var(--sy, 1));
}

path {
    fill: currentColor;
}
</style>

<script lang="ts">
const types: Record<string, { size: number; viewbox: string }> = {
    mdi: {
        size: 24,
        viewbox: "0 0 24 24",
    },
    "simple-icons": {
        size: 24,
        viewbox: "0 0 24 24",
    },
    default: {
        size: 0,
        viewbox: "0 0 0 0",
    },
};

import { defineComponent } from "vue";

export default defineComponent({
    name: "MdiIcon",

    props: {
        type: { type: String, default: "mdi" },
        path: { type: String, required: true },
        size: { type: [String, Number], default: 24 },
        viewbox: String,
        flip: {
            type: String,
            default: "none",
            validator: (value: string) =>
                ["horizontal", "vertical", "both", "none"].includes(value),
        },
        rotate: { type: Number, default: 0 },
    },

    computed: {
        styles() {
            return {
                "--sx": ["both", "horizontal"].includes(this.flip) ? "-1" : "1",
                "--sy": ["both", "vertical"].includes(this.flip) ? "-1" : "1",
                "--r": isNaN(this.rotate) ? this.rotate : this.rotate + "deg",
            };
        },

        defaults() {
            return types[this.type] || types.default;
        },

        sizeValue() {
            return this.size || this.defaults.size;
        },

        viewboxValue() {
            return this.viewbox || this.defaults.viewbox;
        },
    },
});
</script>
