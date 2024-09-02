public static void main(String[] args) {
    BigDecimal number = new BigDecimal("5.5");
    if (number.scale() > 0) {
        throw new IllegalArgumentException("Factorial is only defined for non-negative integers.");
    }
}
