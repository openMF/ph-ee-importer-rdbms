package hu.dpc.phee.operator.business;

import org.springframework.data.jpa.domain.Specification;

import javax.persistence.metamodel.SingularAttribute;
import java.util.Date;

public class TransactionSpecs {

    public static Specification<Transaction> between(SingularAttribute<Transaction, Date> attribute, Date from, Date to) {
        return (root, query, builder) ->
                builder.and(
                        builder.greaterThanOrEqualTo(root.get(attribute), from),
                        builder.lessThanOrEqualTo(root.get(attribute), to)
                );
    }

    public static Specification<Transaction> later(SingularAttribute<Transaction, Date> attribute, Date from) {
        return (root, query, builder) -> builder.greaterThanOrEqualTo(root.get(attribute), from);
    }

    public static Specification<Transaction> earlier(SingularAttribute<Transaction, Date> attribute, Date to) {
        return (root, query, builder) -> builder.lessThanOrEqualTo(root.get(attribute), to);
    }


    public static <T> Specification<Transaction> match(SingularAttribute<Transaction, T> attribute, T input) {
        return (root, query, builder) -> builder.equal(root.get(attribute), input);
    }
}