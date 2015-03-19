package no.westerdals.dbpedia_idx;

public final class Triple {

    public final String subject, predicate, property;
    private final String MD5Title;

    public Triple(String subject, String predicate, String property) {
        this.subject = subject;
        this.predicate = predicate;
        this.property = property;
        this.MD5Title=MD5.hash(this.subject);
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
}