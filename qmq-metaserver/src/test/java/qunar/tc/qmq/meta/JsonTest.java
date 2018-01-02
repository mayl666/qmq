package qunar.tc.qmq.meta;

import qunar.tc.qmq.serializer.Serializer;
import qunar.tc.qmq.serializer.SerializerFactory;

/**
 * @author yunfeng.yang
 * @since 2017/8/31
 */
public class JsonTest {
    public static void main(String[] args) {
        Serializer serializer = SerializerFactory.create();
        A a = new A();
        a.setA("a");

        String serialize = serializer.serialize(a);
        System.out.println(serialize);

        A1 a1 = serializer.deSerialize(serialize, A1.class);
        System.out.println(a1);
    }

    private static class A1 {
        private String a;
        private String b;

        public String getA() {
            return a;
        }

        public void setA(String a) {
            this.a = a;
        }

        public String getB() {
            return b;
        }

        public void setB(String b) {
            this.b = b;
        }

        @Override
        public String toString() {
            return "A1{" +
                    "a='" + a + '\'' +
                    ", b='" + b + '\'' +
                    '}';
        }
    }

    private static class A {
        private String a;

        public String getA() {
            return a;
        }

        public void setA(String a) {
            this.a = a;
        }


        @Override
        public String toString() {
            return "A{" +
                    "a='" + a + '\'' +
                    '}';
        }
    }
}
