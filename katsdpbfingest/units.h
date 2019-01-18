/**
 * @file
 *
 * Compile-time units and quantity types.
 *
 * This has some of the same basic motivation as Boost.Units, but it's not
 * quite the same thing - mainly because it is designed for cases where unit
 * conversion factors are only known at runtime.
 *
 * A @ref unit_system is an ordered set of units, where each is an integer
 * multiple of the previous one. Units are specified by type tags. The ratios
 * are stored in an instance of @ref unit_system.
 *
 * Quantities with the same unit can be used with addition, subtraction,
 * modulo, assignment, and comparison operators. Quantities can also be
 * multiplied if a specialization of @ref unit_product is provided to
 * indicate the unit of the product.
 */

#include <type_traits>
#include <utility>
#include <ostream>

// Forward declarations
template<typename T, typename U> class quantity;
template<typename U, typename T> constexpr quantity<T, U> make_quantity(const T &amount);

namespace detail
{

template<typename T>
static inline T div_round_up(T a, T b)
{
    return (a + b - 1) / b;
}

/**
 * Identity type transformation that fails SFINAE for quantities.
 */
template<typename T>
struct not_quantity_helper
{
    typedef T type;
};

template<typename T, typename U>
struct not_quantity_helper<quantity<T, U>>
{
};

template<typename T>
using not_quantity = not_quantity_helper<typename std::remove_reference<T>::type>;

template<typename T>
using not_quantity_t = typename not_quantity<T>::type;

} // namespace detail

/**
 * A quantity of some unit. This is a type-safe wrapper of @a T, that does not
 * allow implicit conversions to or from @a T. It is intended to be used with
 * integral types.
 */
template<typename T, typename U>
class quantity
{
private:
    T amount;

public:
    constexpr quantity() : amount() {}
    constexpr quantity(const quantity &other) = default;

    /// Retrieve the underlying value
    constexpr const T &get() const { return amount; }

    /// Construct from an integral type
    template<typename T2, typename = typename std::enable_if<std::is_constructible<T, T2>::value>::type>
    constexpr explicit quantity(T2&& x) : amount(std::forward<T2>(x)) {}

    /// Construct from another quantity with the same units
    template<typename T2, typename = typename std::enable_if<std::is_constructible<T, T2>::value>::type>
    constexpr quantity(const quantity<T2, U> &other) : amount(other.get()) {}

#define MAKE_BASIC_OPERATOR(op)                                          \
    template<typename T2>                                                \
    quantity<decltype(std::declval<T>() op std::declval<T2>()), U>       \
    constexpr operator op(const quantity<T2, U> &other) const            \
    {                                                                    \
        return make_quantity<U>(get() op other.get());                   \
    }

#define MAKE_COMPARE_OPERATOR(op)                                        \
    template<typename T2>                                                \
    decltype(std::declval<T>() op std::declval<T2>())                    \
    constexpr operator op(const quantity<T2, U> &other) const            \
    {                                                                    \
        return get() op other.get();                                     \
    }

    // second template arg is just for SFINAE
#define MAKE_ASSIGN_OPERATOR(op)                                         \
    template<typename T2, typename = decltype(std::declval<T &>() op std::declval<T2>())> \
    quantity &operator op(const quantity<T2, U> &other)                  \
    {                                                                    \
        amount op other.get();                                           \
        return *this;                                                    \
    }

    MAKE_BASIC_OPERATOR(+)
    MAKE_BASIC_OPERATOR(-)
    MAKE_BASIC_OPERATOR(%)
    MAKE_COMPARE_OPERATOR(==)
    MAKE_COMPARE_OPERATOR(!=)
    MAKE_COMPARE_OPERATOR(<=)
    MAKE_COMPARE_OPERATOR(>=)
    MAKE_COMPARE_OPERATOR(<)
    MAKE_COMPARE_OPERATOR(>)
    MAKE_ASSIGN_OPERATOR(=)
    MAKE_ASSIGN_OPERATOR(+=)
    MAKE_ASSIGN_OPERATOR(-=)
    MAKE_ASSIGN_OPERATOR(%=)

#undef MAKE_BASIC_OPERATOR
#undef MAKE_COMPARE_OPERATOR
#undef MAKE_ASSIGN_OPERATOR

    /// Multiply by a scalar.
    template<typename T2>
    quantity<decltype(std::declval<T>() * std::declval<detail::not_quantity_t<T2>>()), U>
    constexpr operator *(T2 &&other) const
    {
        return make_quantity<U>(get() * std::forward<T2>(other));
    }

    // The second typename is just for SFINAE
    template<typename T2, typename = decltype(std::declval<T &>() *= std::declval<detail::not_quantity_t<T2>>())>
    quantity &operator *=(T2 &&other)
    {
        amount *= std::forward<T2>(other);
        return *this;
    }

    /**
     * Ratio of quantities, as a unitless scalar. This just uses the
     * underlying division operator, with the usual behaviour for integral
     * types.
     */
    template<typename T2>
    decltype(std::declval<T>() / std::declval<T2>())
    constexpr operator /(const quantity<T2, U> &other) const
    {
        return get() / other.get();
    }

    constexpr explicit operator bool() const
    {
        return (bool) amount;
    }

    constexpr bool operator !() const
    {
        return !amount;
    }

    // Prefix ++
    quantity &operator++()
    {
        ++amount;
        return *this;
    }

    // Prefix --
    quantity &operator--()
    {
        --amount;
        return *this;
    }

    // Postfix ++
    quantity operator++(int)
    {
        return quantity(amount++);
    }

    // Postfix --
    quantity operator--(int)
    {
        return quantity(amount--);
    }
};

/**
 * Helper to construct a quantity while inferring the type.
 */
template<typename U, typename T>
constexpr quantity<T, U> make_quantity(const T &amount)
{
    return quantity<T, U>(amount);
}

/**
 * Multiplication of scalar * quantity.
 */
template<typename T1, typename T2, typename U>
quantity<decltype(std::declval<detail::not_quantity_t<T1>>() * std::declval<T2>()), U>
constexpr operator *(T1&& a, const quantity<T2, U> &b)
{
    return b * a;
}

/**
 * Metaprogramming to determine the results of unit products.
 *
 * Specialize this class with a memory called @c type to indicate the
 * resulting unit type. Note that for heterogeneous products you should
 * specialise in both directions.
 */
template<typename U1, typename U2>
struct unit_product {};

/// Product of two quantities
template<typename T1, typename U1, typename T2, typename U2>
quantity<decltype(std::declval<T1>() * std::declval<T2>()),
         typename unit_product<U1, U2>::type>
constexpr operator *(const quantity<T1, U1> &a, const quantity<T2, U2> &b)
{
    return make_quantity<typename unit_product<U1, U2>::type>(a.get() * b.get());
}

/**
 * Output a quantity to a stream. The unit tag class must define a static
 * member function called @c name.
 */
template<typename T, typename U>
std::ostream &operator<<(std::ostream &os, const quantity<T, U> &q)
{
    return os << q.get() << ' ' << U::name();
}

namespace detail
{

// Whether U is one of the types in Tags
template<typename U, typename... Tags>
class is_one_of : public std::false_type {};

template<typename U, typename T1, typename... Tags>
class is_one_of<U, T1, Tags...> : public is_one_of<U, Tags...> {};

template<typename U, typename... Tags>
class is_one_of<U, U, Tags...> : public std::true_type {};


// Position of U within Tags (or empty class if not in tags)
template<typename U, typename... Tags>
class index_of {};

template<typename U, typename T1, typename... Tags>
class index_of<U, T1, Tags...>
    : public std::integral_constant<int, 1 + index_of<U, Tags...>::value> {};

template<typename U, typename... Tags>
class index_of<U, U, Tags...> : public std::integral_constant<int, 0> {};

/**
 * Back-end of @ref unit_system. This is put into a separate class because it
 * needs several specializations to do its work. The template parameters are
 * the same as for @ref unit_system.
 */
template<typename T, typename U1, typename... Units>
class unit_system_base {};

// At least two units, so at least one ratio
template<typename T, typename U1, typename U2, typename... Units>
class unit_system_base<T, U1, U2, Units...>
{
public:
    typedef unit_system_base<T, U2, Units...> tail_type;

    T ratio1 = 1;      ///< Number of U1's per U2
    tail_type tail;    ///< Remaining ratios (recursively)

    /// Scale factor to convert from @a src to @a dst
    template<typename V1, typename V2>
    constexpr T scale_factor_impl(V1 *src, V2 *dst) const
    {
        // This is the recursive case, when V1 is not U1, so we recurse
        return tail.scale_factor_impl(src, dst);
    }

    template<typename V>
    constexpr T scale_factor_impl(V *src, U1 *) const
    {
        // Base case when dst is U1 but src is not
        return ratio1 * tail.scale_factor_impl(src, (U2 *) nullptr);
    }

    constexpr T scale_factor_impl(U1 *, U1 *) const
    {
        // Base case when both src and dst are U1
        return T(1);
    }

public:
    constexpr unit_system_base() = default;

    template<typename... Args>
    constexpr unit_system_base(typename std::enable_if<sizeof...(Args) == sizeof...(Units), T>::type ratio1,
                               Args&&... ratios)
        : ratio1(ratio1), tail(std::forward<Args>(ratios)...)
    {
    }

    constexpr unit_system_base(T ratio1, unit_system_base<T, U2, Units...> tail)
        : ratio1(ratio1), tail(tail)
    {
    }

    template<typename U>
    constexpr unit_system_base<T, U1, U2, Units..., U> append(T ratio) const
    {
        return unit_system_base<T, U1, U2, Units..., U>(ratio1, tail.template append<U>(ratio));
    }
};

// Base case of recursion: only one unit, so no ratios
template<typename T, typename U>
class unit_system_base<T, U>
{
public:
    constexpr unit_system_base() = default;

    constexpr T scale_factor_impl(U *, U *) const
    {
        return T(1);
    }

    template<typename U2>
    constexpr unit_system_base<T, U, U2> append(T ratio) const
    {
        return unit_system_base<T, U, U2>(ratio);
    }
};

} // namespace detail

/**
 * Collection of units with defined conversion factors.
 *
 * Provides conversions between instances of @c quantity<T,U> for any @a U in
 * @a Units. All calculations are done in the integral type @a T.
 */
template<typename T, typename... Units>
class unit_system : private detail::unit_system_base<T, Units...>
{
public:
    using detail::unit_system_base<T, Units...>::unit_system_base;

    constexpr unit_system(const detail::unit_system_base<T, Units...> &base)
        : detail::unit_system_base<T, Units...>(base)
    {
    }

    /// True type if @a U is one of the units
    template<typename U>
    using is_unit = std::integral_constant<bool, detail::is_one_of<U, Units...>::value>;

    /// Index of @a U amongst the units
    template<typename U>
    using unit_index = std::integral_constant<int, detail::index_of<U, Units...>::value>;

    /**
     * Convert from one quantity to another. This overload only handles
     * conversions from larger to smaller units (conversions from smaller or
     * larger must be done via @ref convert_down or @ref convert_up.
     */
    template<typename U1, typename U2>
    typename std::enable_if<unit_index<U1>::value < unit_index<U2>::value, quantity<T, U1>>::type
    constexpr convert(const quantity<T, U2> &value) const
    {
        return quantity<T, U1>(value.get() * scale_factor<U2, U1>());
    }

    /// No-op conversion.
    template<typename U>
    typename std::enable_if<is_unit<U>::value, quantity<T, U>>::type
    constexpr convert(const quantity<T, U> &value) const
    {
        return value;
    }

    /**
     * Convert from smaller to larger units, rounding down result.
     */
    template<typename U1, typename U2>
    typename std::enable_if<unit_index<U2>::value < unit_index<U1>::value, quantity<T, U1>>::type
    constexpr convert_down(const quantity<T, U2> &value) const
    {
        return quantity<T, U1>(value.get() / scale_factor<U1, U2>());
    }

    /**
     * Convert from smaller to larger units, round up result.
     */
    template<typename U1, typename U2>
    typename std::enable_if<unit_index<U2>::value < unit_index<U1>::value, quantity<T, U1>>::type
    constexpr convert_up(const quantity<T, U2> &value) const
    {
        return quantity<T, U1>(detail::div_round_up(value.get(), scale_factor<U1, U2>()));
    }

    /**
     * Number of @a U2 in each @a U1. Only valid if U1 appears at or after U2
     * in unit list.
     */
    template<typename U1, typename U2>
    typename std::enable_if<unit_index<U1>::value >= unit_index<U2>::value, T>::type
    constexpr scale_factor() const
    {
        return detail::unit_system_base<T, Units...>::scale_factor_impl((U1 *) nullptr, (U2 *) nullptr);
    }

    /**
     * Number of @a U2 in each @a U1, as a quantity. Only valid if U1 appears at or after U2
     * in unit list.
     */
    template<typename U1, typename U2>
    typename std::enable_if<unit_index<U1>::value >= unit_index<U2>::value, quantity<T, U2>>::type
    constexpr convert_one() const
    {
        return quantity<T, U2>(scale_factor<U1, U2>());
    }

    /**
     * Create a new unit system with an additional unit at the end.
     */
    template<typename U>
    constexpr unit_system<T, Units..., U> append(T ratio) const
    {
        return unit_system<T, Units..., U>(
            detail::unit_system_base<T, Units...>::unit_system_base::template append<U>(ratio));
    }
};
