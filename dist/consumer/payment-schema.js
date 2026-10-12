import { z } from "zod";
export const paymentSchema = z.object({
    messageId: z.string().uuid(),
    amount: z.number().positive(),
});
