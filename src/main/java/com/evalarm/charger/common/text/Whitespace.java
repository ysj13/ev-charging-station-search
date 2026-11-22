package com.evalarm.charger.common.text;

/**
 * 문자열 앞뒤 공백 처리.
 *
 * <p>{@link String#trim()}은 U+0020 이하 문자만, {@link String#strip()}은 {@link Character#isWhitespace}만 공백으로 본다.
 * 여기서는 둘 중 어느 쪽이든({@link Character#isWhitespace} 또는 {@link Character#isSpaceChar}) 공백으로 보아,
 * 줄바꿈 없는 공백(U+00A0 등)도 지운다. 공공데이터 응답의 값 앞뒤에 이런 공백이 섞여 와도 같은 값으로 다루기 위해서다.
 */
public final class Whitespace {

    private Whitespace() {
    }

    public static boolean isWhitespace(char c) {
        return Character.isWhitespace(c) || Character.isSpaceChar(c);
    }

    public static String trim(String value) {
        int start = 0;
        int end = value.length();
        while (start < end && isWhitespace(value.charAt(start))) {
            start++;
        }
        while (end > start && isWhitespace(value.charAt(end - 1))) {
            end--;
        }
        return value.substring(start, end);
    }

    public static String trimStart(String value) {
        int start = 0;
        while (start < value.length() && isWhitespace(value.charAt(start))) {
            start++;
        }
        return value.substring(start);
    }

    public static boolean isBlank(String value) {
        for (int i = 0; i < value.length(); i++) {
            if (!isWhitespace(value.charAt(i))) {
                return false;
            }
        }
        return true;
    }
}
