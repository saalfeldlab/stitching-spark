package org.janelia.stitching;

import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.io.Writer;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;

import org.janelia.dataaccess.DataProvider;

import com.google.gson.GsonBuilder;

public class PairwiseShiftsIndexFilter implements Serializable
{
	private static final long serialVersionUID = -2859738504680195410L;

	private final TreeMap< Integer, TreeSet< Integer > > pairwiseShiftsIndexes;

	public PairwiseShiftsIndexFilter( final List< SerializablePairWiseStitchingResult > filteredPairwiseShifts )
	{
		pairwiseShiftsIndexes = new TreeMap<>();
		for ( final SerializablePairWiseStitchingResult shift : filteredPairwiseShifts )
		{
			final SubTilePair tileBoxPair = shift.getSubTilePair();
			final int ind1 = Math.min( tileBoxPair.getA().getIndex(), tileBoxPair.getB().getIndex() );
			final int ind2 = Math.max( tileBoxPair.getA().getIndex(), tileBoxPair.getB().getIndex() );

			if ( !pairwiseShiftsIndexes.containsKey( ind1 ) )
				pairwiseShiftsIndexes.put( ind1, new TreeSet<>() );

			pairwiseShiftsIndexes.get( ind1 ).add( ind2 );
		}
	}

	public PairwiseShiftsIndexFilter( final TreeMap< Integer, TreeSet< Integer > > pairwiseShiftsIndexes )
	{
		this.pairwiseShiftsIndexes = pairwiseShiftsIndexes;
	}

	public List< SerializablePairWiseStitchingResult > filterPairwiseShifts( final List< SerializablePairWiseStitchingResult > pairwiseShifts )
	{
		final List< SerializablePairWiseStitchingResult > filteredPairwiseShifts = new ArrayList<>();
		for ( final SerializablePairWiseStitchingResult shift : pairwiseShifts )
		{
			final SubTilePair tileBoxPair = shift.getSubTilePair();
			final int ind1 = Math.min( tileBoxPair.getA().getIndex(), tileBoxPair.getB().getIndex() );
			final int ind2 = Math.max( tileBoxPair.getA().getIndex(), tileBoxPair.getB().getIndex() );
			if ( pairwiseShiftsIndexes.containsKey( ind1 ) && pairwiseShiftsIndexes.get( ind1 ).contains( ind2 ) )
				filteredPairwiseShifts.add( shift );
		}
		return filteredPairwiseShifts;
	}

	public static void saveToFile( final PairwiseShiftsIndexFilter pairwiseShiftsIndexes, final DataProvider dataProvider, final String filePath ) throws IOException
	{
		try ( final Writer gsonWriter = dataProvider.getJsonWriter( URI.create( filePath ) ) )
		{
			gsonWriter.write( new GsonBuilder().create().toJson( pairwiseShiftsIndexes ) );
		}
	}

	public static PairwiseShiftsIndexFilter loadFromFile( final DataProvider dataProvider, final String filePath ) throws IOException
	{
		try ( final Reader gsonReader = dataProvider.getJsonReader( URI.create( filePath ) ) )
		{
			return new GsonBuilder().create().fromJson( gsonReader, PairwiseShiftsIndexFilter.class );
		}
	}
}
