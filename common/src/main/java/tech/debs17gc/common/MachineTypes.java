package tech.debs17gc.common;

public enum MachineTypes {

	MOLDING("MoldingMachine", 'g'), ASSEMBLY("AssemblyMachine", 'y');

	public final String toString;
	public final char criterion;

	private MachineTypes(String toString, char criterion) {
		this.toString = toString;
		this.criterion = criterion;
	};

}