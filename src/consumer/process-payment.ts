import paymentRepository from "../database/payment.repository.js";

type Payment = {
  messageId: string;
  amount: number;
};

export async function processPayment(payment: Payment): Promise<void> {
  const result = await paymentRepository.processPayment(payment);

  if (result) {
    console.log("Payment processed:", payment.messageId);
  } else {
    console.log("Duplicate payment ignored:", payment.messageId);
  }
}