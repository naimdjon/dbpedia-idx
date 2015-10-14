package no.westerdals.dbpedia_idx;

import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public final class Triple {

    public final String subject, predicate, property;
    public final String MD5Title;
    public final String subjectLowerCased;
    private Set<Category> categories = new HashSet<>();

    public Triple(String ID, String propertyName, String property, Collection<Category> categories) {
        this(ID, propertyName, property);
        setCategories(categories);
    }

    public Triple(String subject, String propertyName, String property) {
        this.subject = subject;
        this.subjectLowerCased = subject.toLowerCase();
        this.predicate = propertyName;
        this.property = property;
        this.MD5Title = MD5.hash(this.subject);
    }

    @Override
    public String toString() {
        return "Triple{" +
                "subject='" + subject + '\'' +
                ", predicate='" + predicate + '\'' +
                ", property='" + property + '\'' +
                '}';
    }

    public String getMD5Title() {
        return MD5Title;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Triple entity = (Triple) o;
        return Objects.equals(subject, entity.subject) &&
                Objects.equals(predicate, entity.predicate) &&
                Objects.equals(property, entity.property) &&
                Objects.equals(MD5Title, entity.MD5Title) &&
                Objects.equals(subjectLowerCased, entity.subjectLowerCased);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subject, predicate, property, MD5Title, subjectLowerCased);
    }

    public Set<Category> getCategories() {
        return categories;
    }

    public void setCategories(Collection<Category> categories) {
        this.categories = ImmutableSet.copyOf(categories);
    }
}