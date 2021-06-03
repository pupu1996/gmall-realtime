package cn.gp1996.gmall.test;

import com.alibaba.otter.canal.protocol.CanalEntry;

public class TestCanalEventType {
    public static void main(String[] args) {
        final CanalEntry.EventType insert = CanalEntry.EventType.valueOf("INSERT");
        System.out.println(insert.getNumber());

        final TestEnum ins1 = TestEnum.valueOf("INS1");
        System.out.println(ins1);
    }


    public static enum TestEnum {
        INS1,INS2
    }
}
