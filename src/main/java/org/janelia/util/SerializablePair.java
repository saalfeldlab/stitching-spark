package org.janelia.util;

import java.io.Serializable;

import net.imglib2.util.Pair;

public class SerializablePair< 
	A extends Serializable, 
	B extends Serializable >
implements 
	Pair< A, B >,
	Serializable
{
	private static final long serialVersionUID = -4611781040293874321L;

	public final A a;
	public final B b;
	
	public SerializablePair( final A a, final B b )
	{
		this.a = a;
		this.b = b;
	}
	
	protected SerializablePair()
	{
		this.a = null;
		this.b = null;
	}

	@Override
	public A getA() 
	{
		return a;
	}

	@Override
	public B getB()
	{
		return b;
	}
	
	@Override
	public String toString()
	{
		return "(" + a.toString() + "," + b.toString() + ")";
	}
}
