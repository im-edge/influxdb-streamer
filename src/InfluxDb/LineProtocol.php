<?php

namespace IMEdge\InfluxDbStreamer\InfluxDb;

use function ksort;
use function strlen;

final class LineProtocol
{
    public static function renderMeasurement($measurement, $tags = [], $fields = [], $timestamp = null): string
    {
        return Escape::measurement($measurement)
            . static::renderTags($tags)
            . static::renderFields($fields)
            . static::renderTimeStamp($timestamp)
            . "\n";
    }

    public static function renderTags($tags): string
    {
        ksort($tags);
        $string = '';
        foreach ($tags as $key => $value) {
            if ($value === null || strlen($value) === 0) {
                continue;
            }
            $string .= ',' . static::renderTag($key, $value);
        }

        return $string;
    }

    public static function renderFields($fields): string
    {
        $string = '';
        foreach ($fields as $key => $value) {
            $string .= ',' . static::renderField($key, $value);
        }
        $string[0] = ' ';

        return $string;
    }

    public static function renderTimeStamp($timestamp): string
    {
        if ($timestamp === null) {
            return '';
        } else {
            return " $timestamp";
        }
    }

    public static function renderTag($key, $value): string
    {
        return Escape::key($key) . '=' . Escape::tagValue($value);
    }

    public static function renderField($key, $value): string
    {

        return Escape::key($key) . '=' . Escape::fieldValue($value);
    }
}
