/* This file is included multiple times from the main code, each time
 * with a different value for TARGET. It thus does *not* have the normal
 * include guard.
 *
 * See https://gcc.gnu.org/wiki/FunctionMultiVersioning for more details.
 * With GCC 6 it would be possible to avoid this multiple include trick
 * and just use the target_clones attribute, but we're targeting GCC 5.4.
 */

#ifdef TARGET
[[gnu::target(TARGET)]]
#endif
uint32_t power_sum(int N, const int8_t *data)
{
    uint32_t accum = 0;
    for (int j = 0; j < N; j++)
    {
        int16_t v = data[j];
        accum += v * v;
    }
    return accum;
}
