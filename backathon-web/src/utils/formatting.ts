const formatter = Intl.NumberFormat();
export const formatNumber = formatter.format;
export { filesize as formatFilesize } from "filesize";

const percentFormatter = Intl.NumberFormat(undefined, {
    style: "percent",
});
export const formatPercent = percentFormatter.format;
