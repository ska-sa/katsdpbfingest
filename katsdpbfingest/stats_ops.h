/* This file is included multiple times from the main code, each time
 * with a different value for TARGET. It thus does *not* have the normal
 * include guard.
 *
 * See https://gcc.gnu.org/wiki/FunctionMultiVersioning for more details.
 * With GCC 6 it would be possible to avoid this multiple include trick
 * and just use the target_clones attribute, but we're targeting GCC 5.4.
 */

// Python extensions are built with -fwrapv, but it interferes with vectorisation
#pragma GCC push_options
#pragma GCC optimize("no-wrapv")

#ifdef TARGET
[[gnu::target(TARGET)]]
#endif
// N is the number of sample values
uint32_t power_sum(int N, const int8_t *data)
{
    uint32_t accum = 0;
    for (int i = 0; i < 2 * N; i++)
    {
        int16_t v = data[i];
        accum += v * v;
    }
    return accum;
}

#ifdef TARGET
[[gnu::target(TARGET)]]
#endif
// N is the number of samples
uint16_t count_saturated(int N, const int8_t *data)
{
    uint16_t ans = 0;
    for (int i = 0; i < N; i++)
    {
        int8_t re = data[2 * i];
        int8_t im = data[2 * i + 1];
        // Using | instead of || helps GCC with autovectorisation
        ans += (re == 127) | (re == -128) | (im == 127) | (im == -128);
    }
    return ans;
}

#pragma GCC pop_options
