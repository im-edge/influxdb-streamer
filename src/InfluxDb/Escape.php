<?php

namespace IMEdge\InfluxDbStreamer\InfluxDb;

use InvalidArgumentException;

use function addcslashes;
use function ctype_digit;
use function is_bool;
use function is_int;
use function is_null;
use function preg_match;

final class Escape
{
    protected const ESCAPE_COMMA_SPACE = ' ,\\';
    protected const ESCAPE_COMMA_EQUAL_SPACE = ' =,\\';
    protected const ESCAPE_DOUBLE_QUOTES = '"\\';
    protected const NULL = 'null';
    protected const TRUE = 'true';
    protected const FALSE = 'false';

    public static function measurement($value): string
    {
        static::assertNoNewline($value);
        return addcslashes($value, self::ESCAPE_COMMA_SPACE);
    }

    public static function key($value): string
    {
        static::assertNoNewline($value);
        return addcslashes($value, self::ESCAPE_COMMA_EQUAL_SPACE);
    }

    public static function tagValue($value): string
    {
        static::assertNoNewline($value);
        return addcslashes($value, self::ESCAPE_COMMA_EQUAL_SPACE);
    }

    public static function fieldValue($value): string
    {
        // Faster checks first
        if (is_float($value)) {
            return (string) $value;
        } elseif (is_int($value) || ctype_digit($value) || preg_match('/^-\d+$/', $value)) {
            return "{$value}";
            // return "{$value}i";
        } elseif (is_bool($value)) {
            return $value ? self::TRUE : self::FALSE;
        } elseif (is_null($value)) {
            return self::NULL;
        } else {
            static::assertNoNewline($value);
            return '"' . addcslashes($value, self::ESCAPE_DOUBLE_QUOTES) . '"';
        }
    }

    protected static function assertNoNewline($value): void
    {
        if (str_contains($value, "\n")) {
            throw new InvalidArgumentException('Newlines are forbidden in InfluxDB line protocol');
        }
    }
}
