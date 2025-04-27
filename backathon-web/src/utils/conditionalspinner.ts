import { h, type SetupContext, toValue } from "vue";
import SpinnerIcon from "@/components/SpinnerIcon.vue";

export function ConditionalSpinner(props: { value: unknown }, context: SetupContext) {
    const val = toValue(props.value);
    if (val) {
        return context.slots.default ? context.slots.default({ value: val }) : null;
    } else {
        return h(SpinnerIcon);
    }
}

ConditionalSpinner.props = {
    value: {
        required: true,
    },
};
