package org.janelia.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import net.imglib2.img.list.ListCursor;
import net.imglib2.img.list.ListImg;
import net.imglib2.type.Type;

public class DirectAccessListImg< T > extends ListImg< T >
{
	public DirectAccessListImg( final long[] dim, final T type )
	{
		super( dim, type );
	}
	
	public DirectAccessListImg( final Collection< T > collection, final long... dim ) 
	{
		super( collection, dim );
	}

	@Override
	public T get( final int index )
	{
		return super.get( index );
	}

	@Override
	public void set( final int index, final T value )
	{
		super.set( index, value );
	}
	
	public List< T > copyContents()
	{
		final List< T > contents = new ArrayList<>();
		for ( int i = 0; i < numPixels; i++ )
			contents.add( get( i ) );
		return contents;
	}
	
	private static < A extends Type< A > > DirectAccessListImg< A > copyWithType( final DirectAccessListImg< A > img )
	{
		final DirectAccessListImg< A > copy = new DirectAccessListImg< A >( img.dimension, img.firstElement().createVariable() );

		final ListCursor< A > source = img.cursor();
		final ListCursor< A > target = copy.cursor();

		while ( source.hasNext() )
			target.next().set( source.next() );

		return copy;
	}

	@SuppressWarnings( { "unchecked", "rawtypes" } )
	@Override
	public DirectAccessListImg< T > copy()
	{
		final T type = firstElement();
		if ( type instanceof Type< ? > )
		{
			final DirectAccessListImg< ? > copy = copyWithType( ( DirectAccessListImg< Type > ) this );
			return ( DirectAccessListImg< T > ) copy;
		}
		return new DirectAccessListImg< T >( copyContents(), dimension );
	}
}
