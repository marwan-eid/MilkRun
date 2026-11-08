import { describe, it, expect } from 'vitest';
import { formatClock, formatEta, formatSlack } from './format';

describe('formatEta', () => {
    it('shows a dash when unknown and "At stop" while delivering', () => {
        expect(formatEta(-1)).toBe('—');
        expect(formatEta(0)).toBe('At stop');
    });

    it('formats seconds and minutes', () => {
        expect(formatEta(9)).toBe('9s');
        expect(formatEta(65)).toBe('1m 05s');
        expect(formatEta(600)).toBe('10m 00s');
    });
});

describe('formatClock', () => {
    it('shows the local hour and minute', () => {
        expect(formatClock(new Date(2026, 10, 8, 14, 5, 30).toISOString())).toBe('14:05');
        expect(formatClock(new Date(2026, 10, 8, 9, 0).toISOString())).toBe('09:00');
    });

    it('returns null for something that is not a time', () => {
        expect(formatClock('soon')).toBeNull();
    });
});

describe('formatSlack', () => {
    it('shows spare time with a plus and lateness as late', () => {
        expect(formatSlack(130)).toBe('+2m 10s');
        expect(formatSlack(0)).toBe('+0s');
        expect(formatSlack(-45)).toBe('45s late');
        expect(formatSlack(-125)).toBe('2m 05s late');
    });

    it('shows a dash when unknown', () => {
        expect(formatSlack(null)).toBe('—');
        expect(formatSlack(undefined)).toBe('—');
    });
});
