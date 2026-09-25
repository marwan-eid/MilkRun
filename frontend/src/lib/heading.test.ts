import { describe, it, expect } from 'vitest';
import { unwrapHeading } from './heading';

describe('unwrapHeading', () => {
    it('turns the short way across north', () => {
        expect(unwrapHeading(350, 10)).toBe(370);
        expect(unwrapHeading(10, 350)).toBe(-10);
    });

    it('keeps accumulated turns', () => {
        expect(unwrapHeading(725, 10)).toBe(730);
        expect(unwrapHeading(-350, 20)).toBe(-340);
    });

    it('does not move for the same heading', () => {
        expect(unwrapHeading(90, 90)).toBe(90);
    });

    it('never turns more than half a circle', () => {
        for (let prev = -720; prev <= 720; prev += 37) {
            for (let h = 0; h < 360; h += 13) {
                const next = unwrapHeading(prev, h);
                expect(Math.abs(next - prev)).toBeLessThanOrEqual(180);
                expect(((next % 360) + 360) % 360).toBeCloseTo(h, 9);
            }
        }
    });
});
