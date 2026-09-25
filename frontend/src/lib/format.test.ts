import { describe, it, expect } from 'vitest';
import { formatEta, formatSlack } from './format';

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
