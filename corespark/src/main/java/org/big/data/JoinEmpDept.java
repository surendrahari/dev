package org.big.data;

/**
 * Created by hadoop on 4/24/17.
 */
public class JoinEmpDept {
    private Integer no;
    private String name;
    private Integer sal;
    private Integer age;
    private Integer deptid;
    private String deptname;

    public Integer getNo () {
        return no;
    }

    public void setNo (Integer no) {
        this.no = no;
    }

    public String getName () {
        return name;
    }

    public void setName (String name) {
        this.name = name;
    }

    public Integer getSal () {
        return sal;
    }

    public void setSal (Integer sal) {
        this.sal = sal;
    }

    public Integer getAge () {
        return age;
    }

    public void setAge (Integer age) {
        this.age = age;
    }

    public Integer getDeptid () {
        return deptid;
    }

    public void setDeptid (Integer deptid) {
        this.deptid = deptid;
    }

    public String getDeptname () {
        return deptname;
    }

    public void setDeptname (String deptname) {
        this.deptname = deptname;
    }

    @Override
    public String toString () {
        return "JoinEmpDept{" +
                "no=" + no +
                ", name='" + name + '\'' +
                ", sal=" + sal +
                ", age=" + age +
                ", deptid=" + deptid +
                ", deptname='" + deptname + '\'' +
                '}';
    }
}
