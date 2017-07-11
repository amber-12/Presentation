
public class StudentDetails {
String sname;
int sage;
float fee;
char gender;

public StudentDetails(String sname,int sage,float fee,char gender)
{
	this.sname=sname;
	this.sage=sage;
	this.fee=fee;
	this.gender=gender;
}

public String getSname() {
	return sname;
}

public void setSname(String sname) {
	this.sname = sname;
}

public int getSage() {
	return sage;
}

public void setSage(int sage) {
	this.sage = sage;
}

public float getFee() {
	return fee;
}

public void setFee(float fee) {
	this.fee = fee;
}

public char getGender() {
	return gender;
}

public void setGender(char gender) {
	this.gender = gender;
}

}
