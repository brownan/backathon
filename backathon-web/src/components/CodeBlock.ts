import { h, type SetupContext } from "vue";

// eslint-disable-next-line @typescript-eslint/no-empty-object-type
export default function CodeBlock(props: {}, context: SetupContext) {
    const slots = context.slots;
    return h(
        "div",
        {
            class: "box is-family-code",
        },
        [slots.default ? slots.default() : null],
    );
}
