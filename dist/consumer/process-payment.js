import paymentRepository from "../database/payment.repository.js";
export async function processPayment(payment) {
    const result = await paymentRepository.processPayment(payment);
    if (result) {
        console.log("Payment processed:", payment.messageId);
    }
    else {
        console.log("Duplicate payment ignored:", payment.messageId);
    }
}
