/**
 * Continuous rotation angle for a new compass heading, so a CSS transition
 * turns the short way: 350° -> 10° becomes 350 -> 370, not 350 -> 10.
 */
export function unwrapHeading(previousAngle: number, heading: number): number {
    const current = ((previousAngle % 360) + 360) % 360;
    const delta = ((heading - current + 540) % 360) - 180;
    return previousAngle + delta;
}
