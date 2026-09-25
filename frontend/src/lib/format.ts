/** "3m 05s" style duration; negative values are shown without sign. */
function duration(totalSeconds: number): string {
    const s = Math.abs(Math.round(totalSeconds));
    const m = Math.floor(s / 60);
    const rest = s % 60;
    return m > 0 ? `${m}m ${String(rest).padStart(2, '0')}s` : `${rest}s`;
}

/** ETA to the next stop: "—" when unknown (-1), "At stop" while delivering (0). */
export function formatEta(seconds: number): string {
    if (seconds < 0) return '—';
    if (seconds === 0) return 'At stop';
    return duration(seconds);
}

/** Slack against the delivery slot: "+2m 10s", "45s late", or "—". */
export function formatSlack(seconds: number | null | undefined): string {
    if (seconds === null || seconds === undefined) return '—';
    return seconds < 0 ? `${duration(seconds)} late` : `+${duration(seconds)}`;
}
