import { pool } from "./connection.js";
async function processPayment({ messageId, amount }) {
    const result = await pool.query({
        text: `
    INSERT INTO payments (message_id, amount, processed_at)
    VALUES ($1, $2, NOW())
    ON CONFLICT (message_id) DO NOTHING
    RETURNING message_id;
  `,
        values: [messageId, amount],
    });
    return result.rowCount === 1;
}
const paymentRepository = {
    processPayment,
};
export default paymentRepository;
