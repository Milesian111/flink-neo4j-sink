public class User {
    private String id;
    private String name;
    private int age;

    // 构造函数、Getter 和 Setter
    public User(String id, String name, int age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    public String getId() { return id; }
    public String getName() { return name; }
    public int getAge() { return age; }
}