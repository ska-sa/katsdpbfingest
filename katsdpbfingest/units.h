/* Compile-time unit checking.
 *
 * This has some of the same basic motivation as Boost.Units, but it's not
 * quite the same thing - mainly because it is designed for cases where unit
 * conversion factors are only known at runtime.
 *
 * A unit system is a set of units, where each is an integer multiple of the
 * previous one. Units are specified with type tags. The ratios are stored in
 * an instance of the unit system.
 */

#include <type_traits>
#include <utility>
#include <ostream>

// Forward declarations
template<typename T, typename U> class quantity;
template<typename U, typename T> constexpr quantity<T, U> make_quantity(const T &amount);

template<typename T, typename U>
class quantity
{
private:
    T amount;

public:
    constexpr quantity() : amount() {}
    constexpr quantity(const quantity &other) = default;

    constexpr const T &get() const { return amount; }

    template<typename T2, typename = typename std::enable_if<std::is_constructible<T, T2>::value>::type>
    constexpr explicit quantity(T2&& x) : amount(std::forward<T2>(x)) {}

    template<typename T2, typename = typename std::enable_if<std::is_convertible<T2, T>::value>::type>
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

#define MAKE_ASSIGN_OPERATOR(op)                                         \
    quantity &operator op(const quantity &other)                         \
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

    template<typename T2>
    quantity<decltype(std::declval<T>() * std::declval<T2>()), U>
    constexpr operator *(T2 &&other) const
    {
        return make_quantity<U>(get() * std::forward<T2>(other));
    }

    template<typename T2, typename = decltype(std::declval<T>() * std::declval<T2>())>
    quantity &operator *=(T2 &&other)
    {
        amount *= std::forward<T2>(other);
        return *this;
    }

    template<typename T2>
    decltype(std::declval<T>() % std::declval<T2>())
    constexpr operator /(const quantity<T2, U> &other) const
    {
        return get() / other.get();
    }

    constexpr explicit operator bool() const
    {
        return (bool) get();
    }

    constexpr bool operator !() const
    {
        return !get();
    }

    quantity &operator++()
    {
        ++amount;
        return *this;
    }

    quantity &operator--()
    {
        --amount;
        return *this;
    }

    quantity operator++(int)
    {
        return quantity(amount++);
    }

    quantity operator--(int)
    {
        return quantity(amount--);
    }
};

template<typename U, typename T>
constexpr quantity<T, U> make_quantity(const T &amount)
{
    return quantity<T, U>(amount);
}

template<typename T1, typename T2, typename U>
quantity<decltype(std::declval<T1>() * std::declval<T2>()), U>
constexpr operator *(T1&& a, const quantity<T2, U> &b)
{
    return b * a;
}

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


template<typename T, typename U1, typename... Units>
class unit_system_base {};

template<typename T, typename U1, typename U2, typename... Units>
class unit_system_base<T, U1, U2, Units...>
{
public:
    typedef U1 head_unit;
    typedef unit_system_base<T, U2, Units...> tail_type;

    T ratio1 = 1;
    tail_type tail;

    // Scale factor to convert from unit src to unit dst
    template<typename V1, typename V2>
    T scale_factor_impl(V1 src, V2 dst) const
    {
        return tail.scale_factor_impl(src, dst);
    }

    template<typename V>
    T scale_factor_impl(V src, U1 dst) const
    {
        return ratio1 * tail.scale_factor_impl(src, U2());
    }

    T scale_factor_impl(U1, U1) const
    {
        return T(1);
    }

public:
    unit_system_base() = default;

    template<typename... Args>
    unit_system_base(typename std::enable_if<sizeof...(Args) == sizeof...(Units), T>::type ratio1,
                     Args&&... ratios)
        : ratio1(ratio1), tail(std::forward<Args>(ratios)...)
    {
    }
};

template<typename T, typename U>
class unit_system_base<T, U>
{
public:
    constexpr T scale_factor_impl(U, U) const
    {
        return T(1);
    }
};

} // namespace detail

template<typename T, typename... Units>
class unit_system : private detail::unit_system_base<T, Units...>
{
public:
    using detail::unit_system_base<T, Units...>::unit_system_base;

    template<typename U>
    using is_unit = std::integral_constant<bool, detail::is_one_of<U, Units...>::value>;

    template<typename U>
    using unit_index = std::integral_constant<int, detail::index_of<U, Units...>::value>;

    template<typename U1, typename U2>
    typename std::enable_if<unit_index<U1>::value < unit_index<U2>::value, quantity<T, U1>>::type
    convert(const quantity<T, U2> &value) const
    {
        return quantity<T, U1>(value.get() * this->scale_factor_impl(U2(), U1()));
    }

    template<typename U1, typename U2>
    typename std::enable_if<unit_index<U2>::value < unit_index<U1>::value, quantity<T, U1>>::type
    convert_down(const quantity<T, U2> &value) const
    {
        return quantity<T, U1>(value.get() / this->scale_factor_impl(U1(), U2()));
    }

    template<typename U1, typename U2>
    typename std::enable_if<unit_index<U2>::value < unit_index<U1>::value, quantity<T, U1>>::type
    convert_up(const quantity<T, U2> &value) const
    {
        T scale = this->scale_factor_impl(U1(), U2());
        return quantity<T, U1>((value.get() + scale - 1) / scale);
    }

    template<typename U>
    typename std::enable_if<is_unit<U>::value, quantity<T, U>>::type
    convert(const quantity<T, U> &value) const
    {
        return value;
    }

    template<typename U1, typename U2>
    typename std::enable_if<unit_index<U1>::value >= unit_index<U2>::value, T>::type
    scale_factor() const
    {
        return detail::unit_system_base<T, Units...>::scale_factor_impl(U1(), U2());
    }

    template<typename U1, typename U2>
    typename std::enable_if<unit_index<U1>::value >= unit_index<U2>::value, quantity<T, U2>>::type
    convert_one() const
    {
        return quantity<T, U2>(scale_factor<U1, U2>());
    }
};
