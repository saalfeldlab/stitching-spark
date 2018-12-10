package net.imglib2.converter;

import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;

public class ClampingConverter< I extends NativeType< I > & RealType< I >, O extends NativeType< O > & RealType< O > > implements Converter< I, O >
{
	private final double minInputValue, maxInputValue;
	private final double minOutputValue, maxOutputValue;
	private final double inputValueRange, outputValueRange;

	public ClampingConverter(
			final double minInputValue, final double maxInputValue,
			final double minOutputValue, final double maxOutputValue )
	{
		this.minInputValue = minInputValue; this.maxInputValue = maxInputValue;
		this.minOutputValue = minOutputValue; this.maxOutputValue = maxOutputValue;

		inputValueRange = maxInputValue - minInputValue;
		outputValueRange = maxOutputValue - minOutputValue;
	}

	@Override
	public void convert( final I input, final O output )
	{
		final double inputValue = input.getRealDouble();
		if ( inputValue <= minInputValue )
		{
			output.setReal( minOutputValue );
		}
		else if ( inputValue >= maxInputValue )
		{
			output.setReal( maxOutputValue );
		}
		else
		{
			final double normalizedInputValue = ( inputValue - minInputValue ) / inputValueRange;
			final double realOutputValue = normalizedInputValue * outputValueRange + minOutputValue;
			output.setReal( realOutputValue );
		}
	}
}
