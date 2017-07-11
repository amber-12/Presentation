import java.util.Collections;
import java.util.TreeSet;
import java.util.Comparator;
public class TreeSetEx {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		TreeSet<Employ> obj=new TreeSet<Employ>(new EmpComp1());
		Employ o=new Employ(100,12,"Amber");
		Employ o1=new Employ(130,32,"Amber");
		Employ o2=new Employ(15,24,"Amber");
		Employ o3=new Employ(12,25,"Amber");
		Employ o4=new Employ(10,26,"Amber");
		Employ o5=new Employ(1,22,"Amber");
		
		obj.add(o);
		obj.add(o1);
		obj.add(o2);
		obj.add(o3);
		obj.add(o4);
		obj.add(o5);
		//Collections.sort(obj, new Employ());

		
		for(Employ E1:obj)
		{
			System.out.println(E1.id+" "+E1.age+" "+E1.name+" ");
		}

	}

}
